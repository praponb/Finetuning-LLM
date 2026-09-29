from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city_length", "id")
w2 = Window.orderBy("city_length", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("city_length").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("rating").between(2.1, 2.2))
    .select(
        "id",
        F.length("city").alias("city_length"),
        F.when(F.col("rating") >= 3.9, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_tier"),
    )
)

result = (
    base
    .select(
        "id",
        "city_length",
        F.lead("city_length", 1).over(w).alias("next_city_length"),
        F.avg("city_length").over(w2).alias("city_length_avg_2_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
