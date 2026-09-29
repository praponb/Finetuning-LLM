from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2025-02-01", "2025-09-03"))
    .select("id", "status", "delivered_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "delivered_date")
)
