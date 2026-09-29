from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-03-30").cast("date")) & (F.year("launch_date") >= 2025))
    .withColumn("tags_count", F.size("tags"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("NONE")))
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_name", "brand", "price", "cost", "tags", "supplier_id")
)
