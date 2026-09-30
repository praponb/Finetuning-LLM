from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "is_active",
        "age",
        F.lead("age").over(w).alias("next_age"),
        F.lag("age").over(w).alias("prev_age"),
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
