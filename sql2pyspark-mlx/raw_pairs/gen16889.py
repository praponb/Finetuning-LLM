from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())

result = (
    df_subscriptions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
    )
)
