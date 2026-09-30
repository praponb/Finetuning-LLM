from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2023-10-01") & (F.col("quantity") == 135))
    .select("id", "last_restock")
    .orderBy(F.col("last_restock").desc(), "id")
    .limit(3)
)
