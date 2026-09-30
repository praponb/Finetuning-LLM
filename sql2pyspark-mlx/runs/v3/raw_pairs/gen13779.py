from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select("id", "status", "cost", F.ntile(3).over(w).alias("quartile"))
)
