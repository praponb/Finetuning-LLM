from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "country",
        "city",
        "rating",
        "supplier_name",
        F.first("supplier_name").over(w).alias("first_supplier_name"),
    )
    .orderBy("id")
)
