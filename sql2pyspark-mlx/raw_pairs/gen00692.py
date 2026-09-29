from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 1.8, "Medium").otherwise("Low").alias("rating_band"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.rating", F.col("c.stock_qty"))
)
