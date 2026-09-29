from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 2).over(w).alias("next_credit_limit"),
    )
)
