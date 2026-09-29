from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") <= 190) & (F.col("cost") >= 530))
    .select("supplier_id")
)
