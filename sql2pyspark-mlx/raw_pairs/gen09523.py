from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-6, -1)

result = (
    df_customers
    .select(
        "*",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.avg("age").over(w2).alias("age_avg_6_prior"),
    )
)
