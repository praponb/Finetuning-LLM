from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_grp = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-6, -1)

result = (
    df_stores
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w_grp))
)
