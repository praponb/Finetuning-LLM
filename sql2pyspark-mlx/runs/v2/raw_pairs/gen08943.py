from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("id", "carrier", "cost", "status", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
)
