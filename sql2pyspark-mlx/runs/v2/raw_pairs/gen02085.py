from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2023-08-01")
    .orderBy(F.col("quantity").desc(), "id")
    .limit(3)
)
