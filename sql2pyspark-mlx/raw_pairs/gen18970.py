from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "country",
        "credit_limit",
        F.round(F.avg("credit_limit").over(w), 2).alias("credit_limit_moving_avg_4"),
    )
)
