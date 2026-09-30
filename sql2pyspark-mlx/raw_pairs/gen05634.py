from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")

latest = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rn",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "CORE").otherwise("OTHER_ZONE").alias("city_zone"),
    )
)
