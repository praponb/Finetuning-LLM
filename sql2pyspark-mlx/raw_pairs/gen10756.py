from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("price").asc_nulls_first(), "id")

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") >= F.lit("2023-02-27").cast("date")))
    .withColumn("brand_prefix", F.substring("brand", 1, 4))
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "cost", "stock_qty", "launch_date", "supplier_id", "brand_prefix")
)
