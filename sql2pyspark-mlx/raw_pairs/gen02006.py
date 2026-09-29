from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-23").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .withColumn("product_name_length", F.length("product_name"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "tags", "product_name_length", "resolved_brand")
)
