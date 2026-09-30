from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("product_cost", "id")
w3 = Window.partitionBy("country")
w4 = Window.orderBy(F.col("rating").asc_nulls_last())
w5 = Window.partitionBy("country").orderBy("product_stock_qty")

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Singapore"]))
    .withColumn("city_country", F.concat("city", F.lit(" "), "country"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.rating",
        "s.city_country",
        F.col("c.cost").alias("product_cost"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

deduped = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isin(["Stark", "Acme"])), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["JP", "TH", "DE"]))
        .select("id", "country")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("product_cost") <= 450)
    .select(
        "country",
        "product_cost",
        "product_stock_qty",
        "latest_rank",
        F.round((F.col("product_cost") / F.sum("product_cost").over(w3)), 4).alias("product_cost_share"),
        F.percent_rank().over(w4).alias("rating_percent_rank"),
        F.rank().over(w5).alias("product_stock_qty_rank"),
    )
)

result = (
    enriched
    .select(
        "country",
        "product_cost",
        "product_stock_qty",
        "latest_rank",
        "product_cost_share",
        F.when(F.col("country").rlike("^(UK|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("product_cost").asc_nulls_first())
)
