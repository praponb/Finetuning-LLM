from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%"))
    .withColumn("city_length", F.length("city"))
    .withColumn("country_supplier_name", F.concat("country", F.lit(" "), "supplier_name"))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "city_length",
        "country_supplier_name",
    )
)
