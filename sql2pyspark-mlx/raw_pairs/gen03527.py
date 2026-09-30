from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED"]))
    .withColumn("prev_cost", F.lag("cost").over(w_grp))
    .withColumn("next_cost", F.lead("cost").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)
