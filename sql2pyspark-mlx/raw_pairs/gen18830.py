from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("credit_limit", "id")
w_recent = Window.partitionBy("segment").orderBy("is_active_flag", "id")
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("segment").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-06").cast("date")) & F.col("segment").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

result = (
    base
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_active_flag",
        F.lag("is_active_flag", 1).over(w_hist).alias("prev_is_active_flag"),
        F.lead("credit_limit", 2, 0).over(w_recent).alias("next_credit_limit"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age"),
        F.first("credit_limit", ignorenulls=True).over(w4).alias("first_known_credit_limit"),
    )
)
