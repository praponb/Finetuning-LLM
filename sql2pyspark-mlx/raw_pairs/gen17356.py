from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "age",
        F.lag("age", 1).over(w).alias("prev_age"),
    )
    .orderBy("age", "prev_age")
)
