from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("price", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("country").orderBy("price", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("country")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "country",
        "city",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 1.7, "Medium").otherwise("Low").alias("rating_band"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Globex"), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Fashion", "Sports"])), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id") == 12), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", F.col("c.price"))
)

result = (
    with_latest
    .select(
        "id",
        "country",
        F.avg("price").over(w2).alias("price_avg_6_prior"),
        F.avg("price").over(w3).alias("price_avg_3_prior"),
        F.round((F.col("price") / F.sum("price").over(w4)), 4).alias("price_share"),
    )
)
