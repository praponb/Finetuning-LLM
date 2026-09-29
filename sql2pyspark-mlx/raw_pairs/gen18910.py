from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "tags", F.upper("product_name").alias("product_name_upper"))
    .orderBy("product_name_upper", F.col("id").desc())
)
