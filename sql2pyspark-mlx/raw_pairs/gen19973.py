from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

deduped = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        F.when(F.col("city").rlike("^(Tokyo|Chiang Mai)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|New York)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("city_zone"),
    )
)
