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
        "age",
        "country",
        "credit_limit",
        F.lead("age", 1).over(w).alias("next_age"),
        F.lag("credit_limit", 1, 0).over(w2).alias("prev_credit_limit"),
    )
)
