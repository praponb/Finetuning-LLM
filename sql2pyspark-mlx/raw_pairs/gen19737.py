from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("signup_date"), "city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "city",
        "credit_limit",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
    )
)
