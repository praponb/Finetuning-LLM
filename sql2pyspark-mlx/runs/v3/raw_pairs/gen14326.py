from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .unionByName(
        df_stores
        .filter(F.month("opened_date").isin([8, 9, 10]))
        .select("city")
    )
    .distinct()
    .orderBy("city")
)
