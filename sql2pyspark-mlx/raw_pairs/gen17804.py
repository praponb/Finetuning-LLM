from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("country").orderBy("rating_rounded", "id")
w4 = Window.partitionBy("country").orderBy("rating_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("product_stock_qty", "id")
w6 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category").orderBy("prev_rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round("rating", 1).alias("rating_rounded"),
        F.when(F.col("rating") >= 3.6, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low").alias("rating_band"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") != "Globex"), "left_anti")
)

latest = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.rating",
        "s.rating_rounded",
        "s.rating_band",
        "s.city_filled",
        F.col("c.category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

scored = (
    with_last
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "rating",
        "city_filled",
        "category",
        F.row_number().over(w_recent).alias("event_seq"),
        F.first("city", ignorenulls=True).over(w4).alias("first_known_city"),
        F.lag("rating", 2, 0).over(w5).alias("prev_rating"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.sum("rating").over(w6).alias("cumulative_rating"),
        F.lead("rating", 1).over(w7).alias("next_rating"),
    )
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "rating",
        "city_filled",
        "event_seq",
        "prev_rating",
        "cumulative_rating",
        F.when(F.col("supplier_name").rlike("^(EuroGoods GmbH|Global Supply)$"), "CORE").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "GROUP_B").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
    .orderBy(F.col("prev_rating").desc_nulls_first())
)
