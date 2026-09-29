from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("age", "id")
w2 = Window.partitionBy(F.month("signup_date"), "city").orderBy("age", "id")

metrics = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "London"]))
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
        F.lag("credit_limit", 1).over(w2).alias("prev_credit_limit"),
    )
)

result = metrics.select("id", "is_active", "credit_limit", "age")
