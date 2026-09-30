from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city") != "Bangkok")
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city_clean",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)
