from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-07-21")
    .select("id", "order_id", "shipped_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "order_id", "shipped_date")
)
