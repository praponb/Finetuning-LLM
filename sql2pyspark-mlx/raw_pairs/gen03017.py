from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-5, -1)

metrics = (
    df_customers
    .select(
        "*",
        F.lag("age").over(w_hist).alias("prev_age"),
        F.lag("credit_limit").over(w_hist).alias("prev_credit_limit"),
        F.lead("credit_limit").over(w_hist).alias("next_credit_limit"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.avg("age").over(w2).alias("age_avg_5_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "prev_age",
        "prev_credit_limit",
    )
    .orderBy(F.col("segment").desc())
)
