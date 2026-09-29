from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select("id", "carrier", "delivered_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "carrier", "delivered_date")
    .orderBy("carrier")
)
