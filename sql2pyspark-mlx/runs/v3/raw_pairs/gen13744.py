from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["FedEx", "Flash", "Kerry"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("delivered_date").desc())
)
