from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("is_jp", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy(F.col("is_jp").desc_nulls_last())
w3 = Window.partitionBy("country")
w4 = Window.orderBy(F.col("age").desc(), "id")
w5 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-09-19").cast("date"))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.last("city", ignorenulls=True).over(w_hist).alias("last_known_city"),
        F.dense_rank().over(w_grp).alias("is_jp_dense_rank"),
        F.min("credit_limit").over(w3).alias("country_min_credit_limit"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("is_jp").over(w5).alias("running_is_jp"),
    )
)
