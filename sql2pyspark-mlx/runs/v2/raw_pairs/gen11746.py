from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("id", "order_id", "shipped_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
