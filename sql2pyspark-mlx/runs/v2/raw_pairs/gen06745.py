from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([7, 9, 10]))
    .select("id", "status", "shipped_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "status", "shipped_date")
)
