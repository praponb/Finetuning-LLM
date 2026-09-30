from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("signup_date") > "2023-11-01")
    .select(
        "id",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
    )
)
