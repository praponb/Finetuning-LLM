from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select("id", "status", "shipped_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "shipped_date")
    .orderBy("status")
)
