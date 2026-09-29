from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("cost").between(35, 66))
    .select("id", "order_id", "cost", F.row_number().over(w).alias("cost_row"))
)
