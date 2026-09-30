from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-06-27"))) & (F.col("cost") > 220))
    .withColumn("launch_date_day", F.dayofmonth("launch_date"))
)

result = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.tags",
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)
