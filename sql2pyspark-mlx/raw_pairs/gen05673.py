from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("credit_limit").desc_nulls_first(), "id")

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "age",
        F.when(F.col("name").rlike("^(David Lee |Pim Chaiyaporn)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("CORE").alias("name_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
