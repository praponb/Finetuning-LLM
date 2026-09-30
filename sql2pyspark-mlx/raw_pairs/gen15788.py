from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([1, 8, 15]))
    .select("id", "carrier", "cost", F.row_number().over(w).alias("cost_row"))
)
