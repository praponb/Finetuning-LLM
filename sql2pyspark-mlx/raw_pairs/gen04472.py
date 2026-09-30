from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy(F.year("signup_date"))

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "signup_date",
        "credit_limit",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
        F.min("credit_limit").over(w2).alias("signup_date_year_min_credit_limit"),
    )
)
