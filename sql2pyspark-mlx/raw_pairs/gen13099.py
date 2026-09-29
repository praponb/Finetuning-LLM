from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("rating_whole_2").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("Berlin")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.floor("rating").alias("rating_whole"),
        F.ceil("rating").alias("rating_whole_2"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

enriched = (
    latest_match
    .filter(F.col("stock_qty") >= 382)
    .select(
        "*",
        F.min("rating").over(w2).alias("city_min_rating"),
        F.last("stock_qty", ignorenulls=True).over(w3).alias("last_known_stock_qty"),
        F.rank().over(w4).alias("rating_whole_2_rank"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") > 1010), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "rating_whole",
        "rating_whole_2",
        "city_filled",
        "price",
    )
    .orderBy("country")
)
