from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 1, 0).over(w).alias("next_credit_limit"),
    )
    .orderBy(F.col("signup_date").desc())
)
