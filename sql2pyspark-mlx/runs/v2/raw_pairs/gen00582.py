from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "quantity")
    .orderBy("id")
    .limit(50)
)
