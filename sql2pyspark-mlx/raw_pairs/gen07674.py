from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select("id", "status", "delivered_date", F.row_number().over(w).alias("row_num"))
)
