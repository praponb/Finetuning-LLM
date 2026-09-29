from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "country",
        "rating",
        "city",
        "supplier_name",
        F.min("rating").over(w).alias("country_min_rating"),
        F.first("supplier_name").over(w2).alias("first_supplier_name"),
    )
    .orderBy(F.col("country_min_rating").desc(), F.col("id").desc())
)
