from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2023)
    .select("id", "carrier", "cost", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "carrier", "cost")
    .orderBy("id")
)
