from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("name").like("%Chai%"))
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
    )
)
