from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select("id", "order_id", "shipped_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "order_id", "shipped_date")
)
