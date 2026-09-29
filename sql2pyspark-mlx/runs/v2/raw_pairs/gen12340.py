from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        F.lead("credit_limit", 2).over(w).alias("next_credit_limit"),
    )
)
