from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2024)
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)
