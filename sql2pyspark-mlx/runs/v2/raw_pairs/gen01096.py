from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "last_restock")
    .orderBy("last_restock", "id")
    .limit(3)
)
