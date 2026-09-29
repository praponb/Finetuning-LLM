from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("segment", "city")
w4 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "country").orderBy(F.col("signup_date").desc(), "id")

base = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

result = (
    base
    .select(
        "*",
        F.ntile(10).over(w).alias("age_bucket"),
        F.lead("age", 1).over(w2).alias("next_age"),
        F.max("age").over(w3).alias("segment_max_age"),
        F.last("city", ignorenulls=True).over(w4).alias("last_known_city"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
