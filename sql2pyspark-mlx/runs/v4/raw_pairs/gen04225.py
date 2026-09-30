from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment", "country")

result = (
    df_customers
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("age_bucket"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_5_prior"),
        F.sum("credit_limit").over(w4).alias("running_credit_limit"),
        F.stddev_samp("credit_limit").over(w5).alias("segment_stddev_samp_credit_limit"),
    )
)
