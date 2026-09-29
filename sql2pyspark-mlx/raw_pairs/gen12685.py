from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("segment")
w3 = Window.orderBy("credit_limit", "id").rowsBetween(-3, -1)
w_part_2 = Window.partitionBy(F.year("signup_date"), "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "city")

result = (
    df_customers
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "is_active",
        F.first("age", ignorenulls=True).over(w).alias("first_known_age"),
        F.max("age").over(w_part).alias("segment_max_age"),
        F.avg("age").over(w3).alias("age_avg_3_prior"),
        F.sum("age").over(w_part_2).alias("running_age"),
        F.min("credit_limit").over(w_recent).alias("signup_date_period_min_credit_limit"),
    )
)
