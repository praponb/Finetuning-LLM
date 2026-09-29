from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "UPS"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "status", "cost", "shipped_date")
    .orderBy(F.col("status").desc())
)
