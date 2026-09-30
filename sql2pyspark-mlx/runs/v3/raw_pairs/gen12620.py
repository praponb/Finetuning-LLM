from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
    )
)
