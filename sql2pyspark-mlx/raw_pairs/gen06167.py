from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 4, 7]))
    .select(
        "id",
        "name",
        "email",
        "country",
        "is_active",
        "credit_limit",
        F.lead("age").over(w_running).alias("next_age"),
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("credit_limit").over(w_running).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_running).alias("prev_credit_limit"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("age").over(w2).alias("age_avg_2"),
    )
)
