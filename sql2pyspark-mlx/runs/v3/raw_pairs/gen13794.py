from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        "country",
        "age",
        F.lag("credit_limit", 1).over(w).alias("prev_credit_limit"),
        F.lead("age", 2).over(w2).alias("next_age"),
    )
    .orderBy(F.col("age").desc())
)
