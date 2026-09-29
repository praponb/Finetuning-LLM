from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "UPS", "FedEx"]))
    .select("id", "status", "delivered_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "delivered_date")
    .orderBy(F.col("id").desc())
)
