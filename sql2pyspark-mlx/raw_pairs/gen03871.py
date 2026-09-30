from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy(F.date_trunc("MONTH", "opened_date"), "city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_6_prior"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
