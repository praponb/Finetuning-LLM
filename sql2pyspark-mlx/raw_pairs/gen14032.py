from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first())

with_windows = (
    df_customers
    .select(
        "*",
        F.avg("credit_limit").over(w_hist).alias("running_avg_credit_limit"),
        F.count("credit_limit").over(w_hist).alias("running_count_credit_limit"),
        F.min("credit_limit").over(w_hist).alias("running_min_credit_limit"),
        F.max("credit_limit").over(w_hist).alias("running_max_credit_limit"),
        F.cume_dist().over(w2).alias("credit_limit_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("signup_date").asc_nulls_first())
)
